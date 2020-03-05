import { Flex } from '@components/common/misc';
import { ISourceStore } from '@stores/source';
import { Switch, Popover } from 'antd';
import { observer } from 'mobx-react';
import React, { Component } from 'react';
import SourceIcon from '@components/icons/sourceIcon';

import {
  SourceDetailsContentText,
  SourceDetailsSourceId,
  SourceDetailsTitleText,
  SourceModalView,
  SourceNameText,
  SourceNameTitle,
  IconColumn,
} from './styles';
import theme from '@css/theme';
import { ISourcesListStore } from '@stores/sourcesList';
import { ButtonSmall } from '@components/common/button';
import { withRouter, RouteComponentProps } from 'react-router';
import { computed } from 'mobx';
import ModalEl from '@components/common/modal';
import { Settings } from '@components/destinationDetails/destinationView/styles';

interface IConfiguredSourcesProps extends RouteComponentProps<any> {
  deleteSource?: (source: ISourceStore) => any;
  source?: ISourceStore;
  history: any;
}

interface ISourceViewState {
  isDeleteModalVisible: boolean;
}

const hoverTitle = 'INFO';
const hoverBody = (
  <p>
    Source has <b>active destinations</b>, <br />
    delete the connections before deleting this source
  </p>
);
const modalTitle = 'CONFIRM DELETION';

@observer
class SourceView extends Component<IConfiguredSourcesProps, ISourceViewState> {
  constructor(props: IConfiguredSourcesProps) {
    super(props);
    this.state = {
      isDeleteModalVisible: false,
    };
  }
  handleOnChange = (checked: boolean, event: Event) => {
    this.props.source!.toggleEnabled();
  };

  handleClickEvent = () => {
    alert('Navigate to settings page');
  };

  deleteSource = (source: ISourceStore) => {
    console.log('source is to be deleted');
    console.log(source.name);
    this.setState({ isDeleteModalVisible: true });
  };

  renderSourceDetails = () => {
    const { source } = this.props;
    const detailsObj: any = {
      'SOURCE ID': source!.id,
      'WRITE KEY': source!.writeKey,
    };
    return (
      <Flex flexDirection="row" spaceBetween className={'m-t-lg'}>
        {Object.keys(detailsObj).map(item => {
          return (
            <SourceDetailsSourceId>
              <SourceDetailsTitleText>{item}</SourceDetailsTitleText>
              <SourceDetailsContentText>
                {detailsObj[item]}
              </SourceDetailsContentText>
            </SourceDetailsSourceId>
          );
        })}
      </Flex>
    );
  };

  @computed get isSourceConnected() {
    return this.props.source!.destinations.length > 0;
  }

  handleModalOk = () => {
    this.setState({ isDeleteModalVisible: false });
    this.props.deleteSource!(this.props.source!);
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
          Are you sure to delete source &nbsp;
          <b>{this.props.source!.name}</b>?{' '}
        </p>
      </div>
    );
  }

  public render() {
    const { source } = this.props;
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
        <SourceModalView>
          <Flex flexDirection="row">
            <Flex flexDirection="column" className={'m-r-sm'}>
              <IconColumn>
                <SourceIcon source={source!.sourceDef.name} />
              </IconColumn>
            </Flex>

            <Flex flexDirection="column" className={'m-l-sm'}>
              <Flex flexDirection="column">
                <SourceNameTitle>{source!.sourceDef.name}</SourceNameTitle>
                <Flex flexDirection="row" spaceBetween>
                  <Flex flexDirection="row">
                    <SourceNameText className={'p-r-md'}>
                      {source!.name}
                    </SourceNameText>
                    <Flex alignItems="center">
                      <Switch
                        defaultChecked={source!.enabled}
                        onChange={this.handleOnChange}
                      />
                    </Flex>
                  </Flex>
                  <Flex alignItems="center"></Flex>
                </Flex>
              </Flex>

              <div>{this.renderSourceDetails()}</div>
            </Flex>
          </Flex>

          <Settings>
            {this.isSourceConnected ? (
              <Popover content={hoverBody} title={hoverTitle}>
                <ButtonSmall
                  disabled={this.isSourceConnected}
                  onClick={() => this.deleteSource(source!)}
                >
                  Delete Source
                </ButtonSmall>
              </Popover>
            ) : (
              <ButtonSmall
                disabled={this.isSourceConnected}
                onClick={() => this.deleteSource(source!)}
              >
                Delete Source
              </ButtonSmall>
            )}
          </Settings>

          {/* <Button onClick={this.handleClickEvent}>
          <Svg name="settings" />
          SETTINGS
          </Button> */}
        </SourceModalView>
      </div>
    );
  }
}

export default withRouter<IConfiguredSourcesProps, any>(SourceView);
