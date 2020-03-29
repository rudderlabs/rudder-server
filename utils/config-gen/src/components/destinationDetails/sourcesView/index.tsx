import { Button, ButtonSmall } from '@components/common/button';
import { Flex } from '@components/common/misc';
import { Label } from '@components/common/typography';
import { ISourceStore } from '@stores/source';
import Svg from '@svg/index';
import { ReactComponent as Delete } from '@svg/delete.svg';
import { Table } from 'antd';
import { observer } from 'mobx-react';
import React, { Component } from 'react';
import { RouteComponentProps, withRouter } from 'react-router-dom';

import {
  DestinationsView,
  DetailsText,
  HeaderView,
  Heading,
  TableViewContainer,
  TitleText,
} from './styles';
import SourceIcon from '@components/icons/sourceIcon';
import theme from '@css/theme';
import { IDestinationStore } from '@stores/destination';
import { IconColumn } from '../destinationView/styles';
import ModalEl from '@components/common/modal';

interface ISourcesListProps extends RouteComponentProps<any> {
  history: any;
  sources?: ISourceStore[];
  destination: any;
  deleteConnection?: (
    source: ISourceStore,
    destination: IDestinationStore,
  ) => any;
}

const modalTitle = 'CONFIRM DELETION';
const columns = [
  { dataIndex: 'sourceIcon', key: 'sourceIcon' },
  {
    dataIndex: 'name',
    key: 'name',
    width: '45%',
  },
  {
    dataIndex: 'status',
    key: 'status',
    width: '45%',
  },
  {
    dataIndex: 'delete',
    key: 'delete',
    width: '10%',
  },
];
@observer
class SourcesView extends Component<ISourcesListProps, any> {
  sourceToBeDeleted: any;
  constructor(props: ISourcesListProps) {
    super(props);
    this.state = {
      dataSource: [],
      destination: undefined,
      isDeleteModalVisible: false,
    };
    // initial config
    this.sourceToBeDeleted = { name: '' };
  }

  handleClickEvent = () => {
    const { history } = this.props;
    const { destination } = this.state;
    history.push(`/sources/connect/${destination.id}`);
  };

  handleRowClick = (record: any, Index: Number) => {
    const { history } = this.props;
    history!.push(`/sources/${record.id}`);
  };

  componentDidMount() {
    const { sources, destination } = this.props;
    const dataSource = sources!.map(source => ({
      ...source,
      sourceIcon: <SourceIcon source={source.sourceDef.name} />,
      status: source.enabled ? 'Enabled' : 'Disabled',
      delete: (
        <ButtonSmall
          onClick={(event: any) => this.deleteConnection(event, source)}
        >
          Disconnect
        </ButtonSmall>
      ),
      key: source.id,
    }));
    this.setState({ dataSource, destination });
  }

  componentWillReceiveProps(nextProps: any) {
    const { sources, destination } = nextProps;
    const dataSource = sources!.map((source: any) => ({
      ...source,
      sourceIcon: <SourceIcon source={source.sourceDef.name} />,
      status: source.enabled ? 'Enabled' : 'Disabled',
      delete: (
        <ButtonSmall
          onClick={(event: any) => this.deleteConnection(event, source)}
        >
          Disconnect
        </ButtonSmall>
      ),
      key: source.id,
    }));
    this.setState({ dataSource, destination });
  }

  deleteConnection = (e: any, source: ISourceStore) => {
    e.preventDefault();
    e.stopPropagation();
    this.sourceToBeDeleted = source;
    this.setState({ isDeleteModalVisible: true });
  };

  locale = {
    emptyText: (
      <Flex flexDirection="column" justifyContentCenter>
        <div className={'m-t-md m-b-md'}>
          <Svg name="ed" />
        </div>
        <Label className={'m-t-xs m-b-xs'}>
          Where do you want to see the data?
        </Label>
        <div className={'m-t-xs m-b-xs'}>
          <p>Add a source for this destination</p>
        </div>
      </Flex>
    ),
  };

  handleModalOk = () => {
    this.setState({ isDeleteModalVisible: false });
    this.props.deleteConnection!(
      this.sourceToBeDeleted!,
      this.props.destination,
    );
  };

  handleModalCancel = () => {
    this.setState({ isDeleteModalVisible: false });
  };

  getModalText() {
    return (
      <div
        style={{
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          height: '10vh',
        }}
      >
        <p>
          Are you sure to delete the connection &nbsp;
          <b>
            {this.sourceToBeDeleted!.name} &nbsp;
            {'<--->'} &nbsp;
            {this.props.destination!.name}
          </b>
          ?
        </p>
      </div>
    );
  }

  public render() {
    const { dataSource } = this.state;
    const { sources } = this.props;
    return (
      <div>
        <ModalEl
          title={modalTitle}
          showModal={this.state.isDeleteModalVisible}
          handleOk={this.handleModalOk}
          handleCancel={this.handleModalCancel}
          okText={'Confirm'}
          element={this.getModalText()}
        />
        <DestinationsView>
          <TableViewContainer>
            <HeaderView>
              <Heading>
                <TitleText>Sources</TitleText>
                <DetailsText>
                  {sources!.filter((source: any) => source.enabled).length}{' '}
                  Enabled
                </DetailsText>
              </Heading>
              <Button onClick={this.handleClickEvent}>
                <Svg name="plus" />
                CONNECT SOURCE
              </Button>
            </HeaderView>
            <Table
              locale={this.locale}
              dataSource={dataSource}
              columns={columns}
              pagination={false}
              showHeader={false}
              onRow={(record, Index) => ({
                onClick: () => this.handleRowClick(record, Index),
              })}
            />
          </TableViewContainer>
        </DestinationsView>
      </div>
    );
  }
}

export default withRouter(SourcesView);
