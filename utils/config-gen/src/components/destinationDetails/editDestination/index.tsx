import * as React from 'react';
import DestinationSettings from '@components/destination/destinationSettings/destinationSettings';
import { IDestinationStore } from '../../../stores/destination';
import { ButtonPrimary } from '@components/common/button';
import {
  HeaderDiv,
  SubHeaderDiv,
  Header,
  Label,
  TextDiv,
} from '@components/common/typography';
import { withTheme } from 'styled-components';
import { Flex } from '@components/common/misc';
import { Popover } from 'antd';

export interface IEditDestinationProps {
  destination?: IDestinationStore;
  onSaveClicked: () => void;
  theme: any;
}

export interface IEditDestinationState {
  config: any;
  enableSaveButton: boolean;
}

class EditDestination extends React.Component<
  IEditDestinationProps,
  IEditDestinationState
> {
  public static defaultProps = {
    onSaveClicked: () => {},
  };

  constructor(props: IEditDestinationProps) {
    super(props);
    this.state = {
      config: {},
      enableSaveButton: false,
    };
  }

  public handleConfigChange = (config: any) => {
    this.setState({ config });
  };

  // public connectTransformation = async () => {
  //   const { selectedTransformation } = this.state;
  //   const { destination } = this.props;
  //   if (selectedTransformation) {
  //     //If 'No transformation needed' is selected, dont connect to any destination
  //     await selectedTransformation.connectToDestination(destination!.id);
  //   } else {
  //     destination!.transformations &&
  //       destination!.transformations[0] &&
  //       (await destination!.transformations[0].disconnectFromDestination(
  //         destination!.id,
  //       ));
  //   }
  // };

  public onSave = async () => {
    const { destination, onSaveClicked } = this.props;
    const { config } = this.state;
    await destination!.updateConfig(config);
    // await this.connectTransformation();
    onSaveClicked();
  };

  public onSelectTransformation = (value: any) => {
    this.setState({
      // selectedTransformation: value,
      // showTransformationSelect: false,
    });
  };

  // public renderEditTransformation = () => {
  //   const { destination, theme } = this.props;
  //   return (
  //     <div>
  //       <Flex
  //         spaceBetween
  //         alignItems="baseline"
  //         className="b-b-grey p-b-sm m-b-md"
  //       >
  //         <SubHeaderDiv color={theme.color.black}>Transformation</SubHeaderDiv>
  //         <SubHeaderDiv
  //           color={theme.color.primary}
  //           onClick={() => this.setState({})}
  //         >
  //           EDIT
  //         </SubHeaderDiv>
  //       </Flex>
  //     </div>
  //   );
  // };

  public render() {
    const { destination, theme } = this.props;
    return (
      <div className="p-l-lg p-r-lg">
        <HeaderDiv className="b-b-grey p-b-lg m-b-sm" color={theme.color.black}>
          Settings
        </HeaderDiv>
        <DestinationSettings
          destName={destination!.destinationDefinition!.name}
          onSettingsChange={this.handleConfigChange}
          setRequirementsState={(reqsState: any) =>
            this.setState({ enableSaveButton: reqsState })
          }
          initialSettings={destination!.config}
        ></DestinationSettings>
        {/* {this.renderEditTransformation()} */}
        <ButtonPrimary onClick={this.onSave}>Save</ButtonPrimary>
      </div>
    );
  }
}

export default withTheme(EditDestination);
