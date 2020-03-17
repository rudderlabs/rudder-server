import { Button, ButtonSmall } from '@components/common/button';
import { Flex } from '@components/common/misc';
import { Label } from '@components/common/typography';
import DestinationIcon from '@components/icons/destinationIcon';
import { IDestinationStore } from '@stores/destination';
import { ReactComponent as Delete } from '@svg/delete.svg';
import Svg from '@svg/index';
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
import theme from '@css/theme';
import { ISourceStore } from '@stores/source';
import { IconColumn } from '@components/destinationDetails/destinationView/styles';
import ModalEl from '@components/common/modal';

interface IConfiguredSourcesProps extends RouteComponentProps<any> {
  history: any;
  destinations?: IDestinationStore[];
  sourceId: string;
  source?: ISourceStore;
  deleteConnection?: (
    source: ISourceStore,
    destination: IDestinationStore,
  ) => any;
}

const modalTitle = 'CONFIRM DELETION';
const columns = [
  { dataIndex: 'destIcon', key: 'destIcon' },
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
class DestinationView extends Component<IConfiguredSourcesProps, any> {
  destinationToBeDeleted: any;
  constructor(props: IConfiguredSourcesProps) {
    super(props);
    this.state = {
      dataSource: [],
      isDeleteModalVisible: false,
    };
    //initial config
    this.destinationToBeDeleted = { name: '' };
  }
  handleClickEvent = () => {
    const { history, sourceId } = this.props;
    history.push(`/destinations/setup?sourceId=${sourceId}`);
  };

  modifyStateAndTable(destinations: any) {
    const dataSource = destinations!.map((destination: any) => ({
      ...destination,
      destIcon: (
        <DestinationIcon destination={destination.destinationDefinition.name} />
      ),
      delete: (
        <ButtonSmall
          onClick={(event: any) => this.deleteConnection(event, destination)}
        >
          Disconnect
        </ButtonSmall>
      ),
      status: destination.enabled ? 'Enabled' : 'Disabled',
      sideIcon: <Svg name="side-arrow" />,
    }));
    this.setState({ dataSource });
  }

  componentDidMount() {
    const { destinations } = this.props;
    this.modifyStateAndTable(destinations);
  }

  componentWillReceiveProps(nextProps: any) {
    const { destinations } = nextProps;
    this.modifyStateAndTable(destinations);
  }
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
          <p>Add a destination for this source</p>
        </div>
      </Flex>
    ),
  };

  handleRowClick = (record: any, Index: Number) => {
    const { history } = this.props;
    history!.push(`/destinations/${record.id}`);
  };

  deleteConnection = (e: any, destination: IDestinationStore) => {
    e.preventDefault();
    e.stopPropagation();
    this.destinationToBeDeleted = destination;
    this.setState({ isDeleteModalVisible: true });
  };

  handleModalOk = () => {
    this.setState({ isDeleteModalVisible: false });
    this.props.deleteConnection!(
      this.props.source!,
      this.destinationToBeDeleted,
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
            {this.props.source!.name} &nbsp;
            {'<--->'} &nbsp;
            {this.destinationToBeDeleted!.name}
          </b>
          ?
        </p>
      </div>
    );
  }

  public render() {
    const { dataSource } = this.state;
    const { destinations } = this.props;
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
                <TitleText>Destinations</TitleText>
                <DetailsText>
                  {
                    destinations!.filter(
                      (destination: any) => destination.enabled,
                    ).length
                  }{' '}
                  Enabled
                </DetailsText>
              </Heading>
              <Button onClick={this.handleClickEvent}>
                <Svg name="plus" />
                ADD DESTINATION
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

export default withRouter<IConfiguredSourcesProps, any>(DestinationView);
